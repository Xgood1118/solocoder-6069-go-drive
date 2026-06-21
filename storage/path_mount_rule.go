package storage

import (
	"go-drive/common/registry"
	"go-drive/common/types"
	"go-drive/common/utils"

	"gorm.io/gorm"
)

type PathMountRuleDAO struct {
	db *DB
}

func NewPathMountRuleDAO(db *DB, ch *registry.ComponentsHolder) *PathMountRuleDAO {
	dao := &PathMountRuleDAO{db: db}
	ch.Add(registry.KeyPathMountRuleDAO, dao)
	return dao
}

func (p *PathMountRuleDAO) GetByDrive(drive string) ([]types.PathMountRule, error) {
	r := make([]types.PathMountRule, 0)
	if e := p.db.C().Find(&r, "`drive` = ?", drive).Error; e != nil {
		return nil, e
	}
	return r, nil
}

func (p *PathMountRuleDAO) GetByPath(drive string, path string) ([]types.PathMountRule, error) {
	r := make([]types.PathMountRule, 0)
	if e := p.db.C().Find(&r, "`drive` = ? AND `path` = ?", drive, path).Error; e != nil {
		return nil, e
	}
	return r, nil
}

func (p *PathMountRuleDAO) GetByDriveAndPath(drive string, path string) ([]types.PathMountRule, error) {
	r := make([]types.PathMountRule, 0)
	paths := utils.PathParentTree(path)
	if e := p.db.C().Where("`drive` = ?", drive).Where("`path` IN ?", paths).Find(&r).Error; e != nil {
		return nil, e
	}
	return r, nil
}

func (p *PathMountRuleDAO) SaveRules(drive string, path string, rules []types.PathMountRule) error {
	now := types.NowMillis()
	return p.db.C().Transaction(func(tx *gorm.DB) error {
		if e := tx.Delete(&types.PathMountRule{}, "`drive` = ? AND `path` = ?", drive, path).Error; e != nil {
			return e
		}
		for _, r := range rules {
			r.Drive = drive
			r.Path = path
			r.CreatedAt = now
			r.UpdatedAt = now
			if e := tx.Create(&r).Error; e != nil {
				return e
			}
		}
		return nil
	})
}

func (p *PathMountRuleDAO) DeleteByPath(drive string, path string) error {
	return p.db.C().Delete(&types.PathMountRule{}, "`drive` = ? AND `path` = ?", drive, path).Error
}

func (p *PathMountRuleDAO) DeleteByDrive(drive string) error {
	return p.db.C().Delete(&types.PathMountRule{}, "`drive` = ?", drive).Error
}

func (p *PathMountRuleDAO) GetTree(drive string) (*types.MountPermissionNode, error) {
	rules, e := p.GetByDrive(drive)
	if e != nil {
		return nil, e
	}

	root := &types.MountPermissionNode{
		Path:        "",
		Drive:       drive,
		IsMountRoot: true,
		Permissions: make([]types.PathMountRule, 0),
		Children:    make([]*types.MountPermissionNode, 0),
	}

	pathMap := make(map[string]*types.MountPermissionNode)
	pathMap[""] = root

	for _, rule := range rules {
		node, ok := pathMap[rule.Path]
		if !ok {
			node = &types.MountPermissionNode{
				Path:        rule.Path,
				Drive:       drive,
				IsMountRoot: rule.Path == "",
				Permissions: make([]types.PathMountRule, 0),
				Children:    make([]*types.MountPermissionNode, 0),
			}
			pathMap[rule.Path] = node

			parentPath := utils.PathParent(rule.Path)
			parent, ok := pathMap[parentPath]
			if !ok {
				parent = &types.MountPermissionNode{
					Path:        parentPath,
					Drive:       drive,
					IsMountRoot: parentPath == "",
					Permissions: make([]types.PathMountRule, 0),
					Children:    make([]*types.MountPermissionNode, 0),
				}
				pathMap[parentPath] = parent
			}
			parent.Children = append(parent.Children, node)
		}
		node.Permissions = append(node.Permissions, rule)
	}

	return root, nil
}

type mountPermItem struct {
	types.PathMountRule
	depth int8
}

func (p *PathMountRuleDAO) GetEffectivePermissions(drive string, path string, subjects []string) (types.Permission, error) {
	rules, e := p.GetByDrive(drive)
	if e != nil {
		return types.PermissionEmpty, e
	}

	subjectMap := make(map[string]bool, len(subjects))
	for _, s := range subjects {
		subjectMap[s] = true
	}

	permMap := make(map[string]*utils.PathTreeNode[*mountPermItem])
	for _, r := range rules {
		if !subjectMap[r.Subject] {
			continue
		}
		sp, ok := permMap[r.Subject]
		if !ok {
			sp = utils.NewPathTreeNodeNonLock[*mountPermItem]("")
			permMap[r.Subject] = sp
		}
		sp.Add(r.Path, &mountPermItem{
			PathMountRule: r,
			depth:         int8(utils.PathDepth(r.Path)),
		})
	}

	items := make([]*mountPermItem, 0)
	for _, sp := range permMap {
		sp.GetCb(path, func(n *utils.PathTreeNode[*mountPermItem]) {
			if n.Data != nil && n.Data.Inherits {
				items = append(items, n.Data)
			}
		})
	}

	return resolveMountPermissions(items), nil
}

func resolveMountPermissions(items []*mountPermItem) types.Permission {
	sortMountPermItems(items)
	acceptedPermission := types.PermissionEmpty
	rejectedPermission := types.PermissionEmpty
	for _, item := range items {
		if item.Policy == types.PolicyAccept {
			acceptedPermission |= item.Permission & ^rejectedPermission
		}
		if item.Policy == types.PolicyReject {
			rejectedPermission |= item.Permission
		}
	}
	return acceptedPermission
}

func sortMountPermItems(items []*mountPermItem) {
	for i := 0; i < len(items)-1; i++ {
		for j := i + 1; j < len(items); j++ {
			if !mountPermLess(items[i], items[j]) {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
}

func mountPermLess(a, b *mountPermItem) bool {
	if a.depth != b.depth {
		return a.depth > b.depth
	}
	if a.Subject == types.AnySubject {
		if b.Subject == types.AnySubject {
			return a.Policy < b.Policy
		}
		return false
	}
	if b.Subject == types.AnySubject {
		return true
	}
	aIsUser := len(a.Subject) > 2 && a.Subject[:2] == "u:"
	bIsUser := len(b.Subject) > 2 && b.Subject[:2] == "u:"
	if aIsUser {
		if bIsUser {
			return a.Policy < b.Policy
		}
		return true
	}
	if bIsUser {
		return false
	}
	return a.Policy < b.Policy
}
